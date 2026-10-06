"""Ordinary Django views: the same code works with Django's WSGI handler."""

from io import BytesIO

from django.contrib.auth.decorators import login_required
from django.http import FileResponse, HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from .models import Note, PrivateNote


def hello(request):
    response = HttpResponse(f"Hello, {request.GET.get('name', 'world')}!")
    response.set_cookie("demo", "working", httponly=True)
    return response


@csrf_exempt
def echo(request):
    # Exempt only this local demo endpoint so curl needs no CSRF token.
    if request.content_type == "application/x-www-form-urlencoded":
        return JsonResponse({"text": request.POST.get("text", "")})
    if request.content_type == "multipart/form-data":
        upload = request.FILES["file"]
        return HttpResponse(upload.read(), content_type="application/octet-stream")
    return HttpResponse(request.body, content_type="application/octet-stream")


def notes(request):
    note = Note.objects.create(text=request.GET.get("text", "hello"))
    return JsonResponse({"text": Note.objects.get(pk=note.pk).text})


def stream(request):
    def chunks():
        yield b"hello "
        yield b"from a synchronous iterator\n"

    return StreamingHttpResponse(chunks(), content_type="text/plain")


def download(request):
    return FileResponse(BytesIO(b"a file from Django\n"), filename="demo.txt")


@login_required
def account(request):
    return render(request, "django_demo/account.html")


@login_required
def me(request):
    return JsonResponse({
        "username": request.user.get_username(),
        "notes": list(PrivateNote.objects.filter(owner=request.user)
                      .order_by("pk").values_list("text", flat=True)),
    })
